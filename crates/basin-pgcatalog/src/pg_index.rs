//! `pg_catalog.pg_index` — which indexes exist, on which table, over which
//! columns, and whether they enforce uniqueness or back a primary key.
//!
//! `docs/migration/df-removal/11-pg-catalog-fidelity.md` calls this and
//! [`crate::pg_constraint`] out together: without them a client that has read
//! [`crate::pg_attribute`] sees a table's columns but nothing about keys or
//! uniqueness — psql's `\d` prints no `"... PRIMARY KEY"` / `"... UNIQUE
//! CONSTRAINT"` footer, and every ORM's introspection (Prisma, Drizzle,
//! SQLAlchemy, ActiveRecord) reports the table as having no primary key at
//! all. A schema that looks complete this way is not.
//!
//! # Where these values come from
//!
//! Checked against a live PostgreSQL 18, not recalled from memory — this
//! project has repeatedly found recall wrong and the server right (see
//! `crates/basin-pgtype/src/operator.rs`'s module docs for the precedent).
//! The queries:
//!
//! ```sql
//! \d pg_index
//!
//! CREATE TABLE idx_test (id serial PRIMARY KEY, a int, b int, UNIQUE(a, b));
//!
//! SELECT indexrelid, indrelid, indnatts, indnkeyatts, indisunique,
//!        indisprimary, indisvalid, indkey, indkey::text, pg_typeof(indkey)
//!   FROM pg_index
//!  WHERE indrelid = 'idx_test'::regclass;
//! --  indexrelid | indrelid | indnatts | indnkeyatts | indisunique | indisprimary | indisvalid | indkey | indkey (text) | pg_typeof
//! --  ...(pkey)  | idx_test |        1 |           1 | t           | t            | t          |      1 | "1"           | int2vector
//! --  ...(uniq)  | idx_test |        2 |           2 | t           | f            | t          |    2 3 | "2 3"         | int2vector
//! ```
//!
//! Confirmed live:
//! - `indexrelid`/`indrelid` are `oid`; `indnatts`/`indnkeyatts` are
//!   `smallint`; `indisunique`/`indisprimary`/`indisvalid` are `boolean`.
//! - `indkey` is `int2vector` — a fixed-size vector of `attnum`s, in index key
//!   order, that both `psql`'s text output and a plain `::text` cast render
//!   as a **space-separated list** (`"1"`, `"2 3"`), not a Postgres array
//!   literal (`{2,3}`) and not comma-separated.
//!
//! # How `indkey` is represented here
//!
//! Arrow has no `int2vector` type, and no fixed-size-vector type that would
//! be a better fit than a list. Rather than reproduce the `psql` text
//! rendering as a `Utf8` string (lossy: a consumer would have to re-parse
//! it), this relation follows the precedent [`crate::pg_proc::PgProc`] set
//! for `proargtypes` (Postgres's other vector type, `oidvector`): `indkey` is
//! an Arrow `List<Int16>`, values in index-key order, queryable and
//! comparable without a string round-trip. A caller that wants the `psql`
//! rendering can still produce it by joining the list with spaces.
//!
//! # What [`CatalogSource`] cannot yet supply
//!
//! [`crate::catalog_source::IndexInfo`] has no notion of an *invalid* index
//! (the state `CREATE INDEX CONCURRENTLY` leaves behind on failure, or a
//! not-yet-finished `REINDEX CONCURRENTLY`), so `indisvalid` is always
//! reported `true` here — there is no way for a caller to construct an
//! `IndexInfo` this relation would report as invalid. Likewise `IndexInfo`
//! does not distinguish key columns from `INCLUDE`d (non-key) columns, so
//! `indnkeyatts` always equals `indnatts`; a real `pg_index` row for `CREATE
//! INDEX ... (a) INCLUDE (b)` would report `indnatts = 2, indnkeyatts = 1`,
//! which this crate cannot currently represent. Both are `IndexInfo` gaps,
//! not a choice made in this file.
//!
//! # Column audit against live PostgreSQL 18.2 (see crate-level task docs)
//!
//! `SELECT attname, atttypid::regtype, attnum, attnotnull FROM pg_attribute
//! WHERE attrelid = 'pg_catalog.pg_index'::regclass AND attnum > 0 ORDER BY
//! attnum` against a live server reports **21** columns; this relation
//! previously implemented 8 of them, three (`indisprimary`, `indisvalid`,
//! `indkey`) in the wrong `attnum` position (each had skipped over columns
//! this file did not yet implement, rather than reserving their slots).
//! Fixed here. The remaining columns, in real `attnum` order:
//!
//! - `indnullsnotdistinct`, `indisexclusion`, `indimmediate`,
//!   `indisclustered`, `indcheckxmin`, `indisready`, `indislive`,
//!   `indisreplident` — all `boolean`, `NOT NULL`. None of these has a
//!   corresponding `IndexInfo` field, but each has a single value real
//!   Postgres reports for *every* index this crate can currently construct
//!   (a plain, non-deferrable, non-exclusion, freshly-built, non-replica-
//!   identity index) — confirmed live against a fresh two-column index:
//!   `indimmediate`, `indisready`, `indislive` are `t`; the rest are `f`.
//!   Added with those literal defaults and a doc comment each, per this
//!   crate's placeholder convention (see `indisvalid` above).
//! - `indoption` — `int2vector`, `NOT NULL`: per-key-column sort option bits
//!   (`DESC`, `NULLS FIRST`). Live-verified `0` (ascending, nulls last) for
//!   every key column of a plain `CREATE INDEX` with no `ASC`/`DESC`/`NULLS`
//!   clause — the only kind of index `IndexInfo` can represent. Added as a
//!   placeholder `List<Int16>` of zeros, one per `indkey` entry.
//! - `indcollation`, `indclass` — both `oidvector`, `NOT NULL`: the
//!   collation and operator class *actually in use* per key column.
//!   Live-verified these are **not** uniformly defaultable the way the
//!   booleans above are — `indcollation` was `0` for an `int` column but
//!   `100` for a `text` column in the same index, and `indclass` is always a
//!   real, non-zero operator-class oid (`1978` = `int4_ops`, `3126` = a text
//!   opclass), never `0`. Computing either correctly requires each key
//!   column's declared type, which `IndexInfo` does not carry. **Not added**
//!   — a placeholder here would be a fabricated value, not a documented
//!   default, and this crate's task is explicit that positional readers of
//!   `indkey`-shaped columns must not be handed plausible-looking noise.
//! - `indexprs`, `indpred` — both `pg_node_tree`, **nullable**: the parsed
//!   expressions for an expression index / the predicate for a partial
//!   index. `IndexInfo` has no notion of either (it only carries key
//!   `attnum`s), so every index this crate can construct is a plain,
//!   non-expression, non-partial index — for which the real, correct value
//!   of both columns is `NULL`, not a fabricated one. Added as nullable
//!   `Utf8` columns, always `NULL`.
//!
//! `basin-pgtype`'s `oid` module defines `INT2VECTOR`, `OIDVECTOR` and
//! `PG_NODE_TREE` constants, but its `physical()` mapping (`lib.rs`) has no
//! match arm for any of them — a generic `PgType -> DataType` lookup for a
//! real `int2vector`/`oidvector`/`pg_node_tree` column fails there. This
//! relation, like [`crate::pg_proc`] for `proargtypes`, sidesteps that by
//! hand-building the Arrow `List` type directly rather than going through
//! `physical()`. Worth noting for whoever owns `basin-pgtype/src/lib.rs`;
//! out of scope to fix from this file.

use std::sync::Arc;

use arrow_array::{
    builder::{Int16Builder, ListBuilder},
    BooleanArray, Int16Array, ListArray, RecordBatch, UInt32Array,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};

use crate::{
    catalog_source::{CatalogSource, IndexInfo},
    error::Error,
    predicate::{Predicate, Value},
};

/// This index's value for `column`, or `None` if `column` is not one of this
/// relation's columns, and not the list-typed one (`indkey`) handled
/// separately by the caller.
fn value(idx: &IndexInfo, column: &str) -> Option<Value> {
    Some(match column {
        "indexrelid" => Value::Oid(idx.oid),
        "indrelid" => Value::Oid(idx.table_oid),
        "indnatts" => Value::Int(idx.column_attnums.len() as i64),
        "indnkeyatts" => Value::Int(idx.column_attnums.len() as i64),
        "indisunique" => Value::Bool(idx.is_unique),
        // `IndexInfo` has no notion of `NULLS NOT DISTINCT` — live-verified
        // `false` (nulls distinct, the default) for every plain index.
        "indnullsnotdistinct" => Value::Bool(false),
        "indisprimary" => Value::Bool(idx.is_primary),
        // `IndexInfo` cannot represent `EXCLUDE` constraints.
        "indisexclusion" => Value::Bool(false),
        // `IndexInfo` has no notion of a deferrable index; live-verified
        // `true` (immediate) for every plain index.
        "indimmediate" => Value::Bool(true),
        // `IndexInfo` has no notion of `CLUSTER`; a fresh index is never
        // clustered.
        "indisclustered" => Value::Bool(false),
        // `IndexInfo` has no notion of an invalid index — see the module
        // docs — so every index this crate reports is valid.
        "indisvalid" => Value::Bool(true),
        // `indcheckxmin` reflects a build-time MVCC detail `IndexInfo`
        // cannot express; live-verified `false` for every plain index.
        "indcheckxmin" => Value::Bool(false),
        // `IndexInfo` cannot represent an in-progress `CREATE INDEX
        // CONCURRENTLY`; every index this crate reports is finished and
        // ready — live-verified `true`.
        "indisready" => Value::Bool(true),
        // `IndexInfo` cannot represent an in-progress `DROP INDEX
        // CONCURRENTLY`; every index this crate reports is live —
        // live-verified `true`.
        "indislive" => Value::Bool(true),
        // `IndexInfo` has no notion of `ALTER TABLE ... REPLICA IDENTITY
        // USING INDEX`; live-verified `false` for every plain index.
        "indisreplident" => Value::Bool(false),
        // `indkey` and `indoption` are list columns; no scalar `Value`
        // represents either, so a predicate naming them is rejected by the
        // schema check in `scan` rather than reaching here. `indexprs` and
        // `indpred` are always `NULL` (see module docs) and likewise have no
        // meaningful scalar predicate value.
        _ => return None,
    })
}

/// `pg_catalog.pg_index`.
#[derive(Debug, Default, Clone, Copy)]
pub struct PgIndex;

impl PgIndex {
    fn arrow_schema() -> SchemaRef {
        let int16_list = || DataType::List(Arc::new(Field::new("item", DataType::Int16, true)));
        Arc::new(Schema::new(vec![
            Field::new("indexrelid", DataType::UInt32, false),
            Field::new("indrelid", DataType::UInt32, false),
            Field::new("indnatts", DataType::Int16, false),
            Field::new("indnkeyatts", DataType::Int16, false),
            Field::new("indisunique", DataType::Boolean, false),
            Field::new("indnullsnotdistinct", DataType::Boolean, false),
            Field::new("indisprimary", DataType::Boolean, false),
            Field::new("indisexclusion", DataType::Boolean, false),
            Field::new("indimmediate", DataType::Boolean, false),
            Field::new("indisclustered", DataType::Boolean, false),
            Field::new("indisvalid", DataType::Boolean, false),
            Field::new("indcheckxmin", DataType::Boolean, false),
            Field::new("indisready", DataType::Boolean, false),
            Field::new("indislive", DataType::Boolean, false),
            Field::new("indisreplident", DataType::Boolean, false),
            Field::new("indkey", int16_list(), false),
            // `indcollation` (attnum 17) and `indclass` (18), both
            // `oidvector`, are omitted. `indclass` is the operator class per
            // key column, which depends on the column's type and the index
            // access method — `IndexInfo` carries neither, and there is no
            // default that is right rather than merely plausible.
            //
            // The consequence is worth stating plainly, because the rest of
            // this file was just reordered precisely so position is faithful:
            // omitting two columns means everything after them sits two slots
            // early. `indoption` is at 17 here and 19 in Postgres. A reader
            // going by NAME is fine; one going by position is not, and this is
            // the one place in this relation where that is still true.
            // Filling them with zeros would fix the arithmetic and lie about
            // the data, which is the worse of the two.
            Field::new("indoption", int16_list(), false),
            Field::new("indexprs", DataType::Utf8, true),
            Field::new("indpred", DataType::Utf8, true),
        ]))
    }
}

impl crate::SystemView for PgIndex {
    fn name(&self) -> &str {
        "pg_index"
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
                    relation: "pg_index",
                    column: p.column().to_string(),
                });
            }
        }

        let rows: Vec<IndexInfo> = catalog
            .tables()
            .iter()
            .flat_map(|t| catalog.indexes(t.oid))
            .filter(|idx| {
                pushed
                    .iter()
                    .all(|p| p.matches(value(idx, p.column()).as_ref()))
            })
            .collect();

        let indexrelids: UInt32Array = rows.iter().map(|r| r.oid.get()).collect();
        let indrelids: UInt32Array = rows.iter().map(|r| r.table_oid.get()).collect();
        let indnatts: Int16Array = rows.iter().map(|r| r.column_attnums.len() as i16).collect();
        let indnkeyatts: Int16Array = rows.iter().map(|r| r.column_attnums.len() as i16).collect();
        let indisuniques: BooleanArray = rows.iter().map(|r| r.is_unique).collect();
        // Placeholder defaults — see the module docs for why each literal is
        // the real value Postgres reports for every index this crate can
        // currently construct, not a guess.
        let indnullsnotdistincts: BooleanArray = rows.iter().map(|_| false).collect();
        let indisprimaries: BooleanArray = rows.iter().map(|r| r.is_primary).collect();
        let indisexclusions: BooleanArray = rows.iter().map(|_| false).collect();
        let indimmediates: BooleanArray = rows.iter().map(|_| true).collect();
        let indisclustereds: BooleanArray = rows.iter().map(|_| false).collect();
        let indisvalids: BooleanArray = rows.iter().map(|_| true).collect();
        let indcheckxmins: BooleanArray = rows.iter().map(|_| false).collect();
        let indisreadys: BooleanArray = rows.iter().map(|_| true).collect();
        let indislives: BooleanArray = rows.iter().map(|_| true).collect();
        let indisreplidents: BooleanArray = rows.iter().map(|_| false).collect();

        let mut indkey_builder = ListBuilder::new(Int16Builder::new());
        let mut indoption_builder = ListBuilder::new(Int16Builder::new());
        for r in &rows {
            for attnum in &r.column_attnums {
                indkey_builder.values().append_value(*attnum);
                // Ascending, nulls-last (`0`) — the only sort option
                // `IndexInfo` can express; see module docs.
                indoption_builder.values().append_value(0);
            }
            indkey_builder.append(true);
            indoption_builder.append(true);
        }
        let indkeys: ListArray = indkey_builder.finish();
        let indoptions: ListArray = indoption_builder.finish();

        // `IndexInfo` cannot represent expression indexes or partial-index
        // predicates, so the real value of both is `NULL` for every index
        // this crate constructs — see module docs.
        let indexprs = arrow_array::StringArray::from(vec![None::<&str>; rows.len()]);
        let indpred = arrow_array::StringArray::from(vec![None::<&str>; rows.len()]);

        Ok(RecordBatch::try_new(
            schema,
            vec![
                Arc::new(indexrelids),
                Arc::new(indrelids),
                Arc::new(indnatts),
                Arc::new(indnkeyatts),
                Arc::new(indisuniques),
                Arc::new(indnullsnotdistincts),
                Arc::new(indisprimaries),
                Arc::new(indisexclusions),
                Arc::new(indimmediates),
                Arc::new(indisclustereds),
                Arc::new(indisvalids),
                Arc::new(indcheckxmins),
                Arc::new(indisreadys),
                Arc::new(indislives),
                Arc::new(indisreplidents),
                Arc::new(indkeys),
                Arc::new(indoptions),
                Arc::new(indexprs),
                Arc::new(indpred),
            ],
        )?)
    }
}

#[cfg(test)]
mod tests {
    use arrow_array::{Array, BooleanArray, Int16Array, ListArray, UInt32Array};

    use super::*;
    use crate::{
        catalog_source::{IndexInfo, RelKind, TableInfo},
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

    fn indkey_row(batch: &RecordBatch, i: usize) -> Vec<i16> {
        let list = batch
            .column(batch.schema().index_of("indkey").unwrap())
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        list.value(i)
            .as_any()
            .downcast_ref::<Int16Array>()
            .unwrap()
            .values()
            .to_vec()
    }

    /// `widgets` has a unique primary key on `id` (one attnum) and a
    /// non-unique, non-primary index on `(a, b)` (two attnums, in order) —
    /// enough to exercise `indisunique`/`indisprimary` distinguishing and
    /// `indkey` ordering. `gadgets` has no indexes at all.
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
            .with_index(IndexInfo {
                oid: Oid(16389),
                table_oid: Oid(16385),
                name: "widgets_pkey".to_string(),
                is_unique: true,
                is_primary: true,
                column_attnums: vec![1],
            })
            .with_index(IndexInfo {
                oid: Oid(16391),
                table_oid: Oid(16385),
                name: "widgets_a_b_idx".to_string(),
                is_unique: false,
                is_primary: false,
                column_attnums: vec![2, 3],
            })
    }

    #[test]
    fn name_is_pg_index() {
        assert_eq!(PgIndex.name(), "pg_index");
    }

    /// Pins the exact column layout (name, order, nullability) against live
    /// PostgreSQL 18.2's `pg_attribute` for `pg_index`, so a future edit
    /// cannot silently reorder, rename, or flip nullability on a column.
    /// `indcollation`/`indclass` are deliberately absent — see module docs.
    #[test]
    fn schema_matches_live_postgres_column_layout() {
        let schema = PgIndex.schema();
        let got: Vec<(&str, bool)> = schema
            .fields()
            .iter()
            .map(|f| (f.name().as_str(), f.is_nullable()))
            .collect();
        assert_eq!(
            got,
            vec![
                ("indexrelid", false),
                ("indrelid", false),
                ("indnatts", false),
                ("indnkeyatts", false),
                ("indisunique", false),
                ("indnullsnotdistinct", false),
                ("indisprimary", false),
                ("indisexclusion", false),
                ("indimmediate", false),
                ("indisclustered", false),
                ("indisvalid", false),
                ("indcheckxmin", false),
                ("indisready", false),
                ("indislive", false),
                ("indisreplident", false),
                ("indkey", false),
                ("indoption", false),
                ("indexprs", true),
                ("indpred", true),
            ]
        );
    }

    /// The entire point of this crate, applied to `pg_index`: a predicate on
    /// `indrelid` must narrow to exactly one table's indexes.
    #[test]
    fn pushed_indrelid_predicate_narrows_to_one_tables_indexes() {
        let full = PgIndex.scan(&catalog(), &[]).unwrap();
        assert_eq!(full.num_rows(), 2, "sanity: widgets has two indexes");

        let filtered = PgIndex
            .scan(&catalog(), &[Predicate::eq("indrelid", Oid(16385))])
            .unwrap();
        assert_eq!(filtered.num_rows(), 2);
        for relid in col_u32(&filtered, "indrelid") {
            assert_eq!(relid, 16385);
        }
    }

    /// `indisprimary` is true exactly for the primary key index, not for
    /// every unique index.
    #[test]
    fn indisprimary_is_true_only_for_the_primary_key() {
        let batch = PgIndex.scan(&catalog(), &[]).unwrap();
        let oids = col_u32(&batch, "indexrelid");
        let primaries = col_bool(&batch, "indisprimary");
        let uniques = col_bool(&batch, "indisunique");

        let by_oid: std::collections::HashMap<u32, (bool, bool)> = oids
            .into_iter()
            .zip(primaries.into_iter().zip(uniques))
            .collect();

        assert_eq!(by_oid[&16389], (true, true), "widgets_pkey");
        assert_eq!(
            by_oid[&16391],
            (false, false),
            "widgets_a_b_idx is neither primary nor unique"
        );
    }

    /// `indkey` carries the indexed columns' `attnum`s in index-key order.
    #[test]
    fn indkey_carries_attnums_in_order() {
        let batch = PgIndex
            .scan(&catalog(), &[Predicate::eq("indexrelid", Oid(16391))])
            .unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(indkey_row(&batch, 0), vec![2, 3]);
    }

    /// `indnatts`/`indnkeyatts` reflect the number of indexed columns.
    #[test]
    fn indnatts_reflects_column_count() {
        let batch = PgIndex
            .scan(&catalog(), &[Predicate::eq("indexrelid", Oid(16391))])
            .unwrap();
        let indnatts = batch
            .column(batch.schema().index_of("indnatts").unwrap())
            .as_any()
            .downcast_ref::<Int16Array>()
            .unwrap();
        let indnkeyatts = batch
            .column(batch.schema().index_of("indnkeyatts").unwrap())
            .as_any()
            .downcast_ref::<Int16Array>()
            .unwrap();
        assert_eq!(indnatts.value(0), 2);
        assert_eq!(indnkeyatts.value(0), 2);
    }

    /// A table with no indexes yields no rows — not an error.
    #[test]
    fn table_with_no_indexes_yields_no_rows() {
        let batch = PgIndex
            .scan(&catalog(), &[Predicate::eq("indrelid", Oid(16390))])
            .unwrap();
        assert_eq!(batch.num_rows(), 0);
    }

    #[test]
    fn empty_catalog_yields_zero_rows() {
        let batch = PgIndex.scan(&MockCatalog::new(), &[]).unwrap();
        assert_eq!(batch.num_rows(), 0);
    }

    /// A predicate naming a column `pg_index` does not have is an error.
    #[test]
    fn predicate_on_unknown_column_is_an_error() {
        let err = PgIndex
            .scan(&catalog(), &[Predicate::eq("nope", 1i64)])
            .unwrap_err();
        assert_eq!(
            err,
            Error::UnknownColumn {
                relation: "pg_index",
                column: "nope".to_string(),
            }
        );
    }
}
