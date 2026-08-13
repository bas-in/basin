//! Mechanical catalog-fidelity check against a live PostgreSQL 18.2.
//!
//! # Why this exists
//!
//! Eight of this crate's relations were audited by hand against a live
//! server; seven were wrong in the exact same way — correct column *names*,
//! correct *types*, wrong **positions**, because columns this crate does not
//! implement were skipped over silently instead of having their `attnum`
//! slot reserved (see `pg_index`'s module docs for the worked example, and
//! `docs/migration/df-removal/11-pg-catalog-fidelity.md`). Every existing
//! by-name unit test (e.g. each relation's own
//! `schema_matches_live_postgres_column_layout`) passed the whole time,
//! because those tests pin an expected `Vec` that a human transcribed by
//! hand from the same audit that got the position wrong — they check the
//! crate's schema against itself, not against the server.
//!
//! This harness closes that gap by generating the expected layout from the
//! server directly, for every relation the crate registers, every run.
//!
//! # The subset-but-ordered design decision
//!
//! Basin deliberately implements a *subset* of most `pg_catalog` relations'
//! real columns (see e.g. `pg_index`'s module docs on `indcollation`/
//! `indclass`). So this harness must not — and does not — require every real
//! column to be present. What it enforces is narrower and is exactly the
//! property the manual audit found broken:
//!
//! > Take the real columns in `attnum` order. Take this crate's implemented
//! > columns in the order `arrow_schema()` declares them. Match implemented
//! > columns to real columns **by name**. The matched real `attnum`s, read
//! > in the order the crate declares them, must be strictly increasing.
//!
//! In other words: the implemented columns must form an **order-preserving
//! subsequence** of the real, `attnum`-ordered column list. A relation may
//! skip columns freely; it may not reorder the ones it keeps relative to one
//! another. This is precisely the invariant the seven broken relations
//! violated — each packed its implemented columns consecutively (attnum
//! `1, 2, 3, ...` in the file) while the real columns they corresponded to
//! were interleaved with columns the crate omits, so two adjacent fields in
//! `arrow_schema()` could map to real attnums that were *not* adjacent, or
//! worse, not increasing.
//!
//! A stricter rule (e.g. "the crate's Nth implemented column must be the
//! real relation's Nth column") is wrong on its face — it would forbid the
//! subsetting this crate does deliberately and by design (see `pg_index`'s
//! `indcollation`/`indclass` omission). A looser rule (e.g. "every
//! implemented column exists somewhere in the real relation, order
//! unchecked") is exactly the by-name check that already existed per-file
//! and already missed the bug seven times. Order-preserving-subsequence is
//! the tightest rule that doesn't reject legitimate subsetting.
//!
//! This harness additionally flags (as a hard failure, not just a metric)
//! any implemented column whose name does not exist among the real columns
//! at all (a fabricated column), any nullability mismatch (`attnotnull` vs.
//! the field's `is_nullable()`), and any **type** mismatch (the declared
//! Arrow `DataType` vs. an accepted representation of the real column's
//! Postgres type — see [`allowed_arrow_types`]), since all three are cheap to
//! check here and are the same class of "quietly wrong" bug.
//!
//! # What this harness does NOT check
//!
//! Only the *shape* of each relation, plus the row *content* of [`PgType`]
//! (see [`diff_pg_type_rows`]). The row content of the other
//! relations whose data is static and hand-transcribed — `pg_am`, `pg_proc`
//! (a view over `basin-pgtype`'s function table) — is still unverified
//! against a live server here. `pg_cast` and `pg_operator` are views over
//! `basin-pgtype`'s cast/operator tables, which `basin-pgtype`'s own
//! `tests/type_matrix.rs` does check cell-by-cell against live; the
//! catalog-dependent relations have no static content to check.
//!
//! # Skip behavior
//!
//! Like every other live-server test in this repo (see
//! `crates/basin-engine/tests/differential_pg.rs`,
//! `tests/integration/tests/differential_pg.rs`), this test reads
//! `PG_DIFF_TEST_DSN` and skips cleanly — does not fail the build — when it
//! is unset. Per `scripts/check-differential.sh`'s hard-won lesson, a skip
//! must never *look* like a pass: this binary is `harness = false` (see
//! `Cargo.toml`'s `[[test]]` entry) specifically so its output is never
//! captured-and-hidden by `libtest` the way a normal `#[test]`'s stdout is
//! on success. Every run — skip or real — prints an explicit, unambiguous
//! banner naming which case happened and how many relations were actually
//! checked (zero, on a skip). `scripts/check-differential.sh` is the correct
//! model for a *wrapping* script that wants to fail closed when the DSN is
//! absent; this file does not attempt to be that script, only to be honest
//! about what it did when run directly.
//!
//! # Dependency placement
//!
//! `tokio` + `tokio-postgres` live in `[dev-dependencies]` only — this
//! crate's production dependency graph is exactly `arrow-array`,
//! `arrow-schema`, `basin-pgtype` (see `Cargo.toml`'s module docs), and nothing
//! here changes that.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{Array, Int16Array, StringArray, UInt32Array};
use arrow_schema::{DataType, Field, SchemaRef};
use basin_pgcatalog::mock::MockCatalog;
use basin_pgcatalog::{
    pg_am::PgAm, pg_attrdef::PgAttrDef, pg_attribute::PgAttribute, pg_cast::PgCast,
    pg_class::PgClass, pg_constraint::PgConstraint, pg_depend::PgDepend,
    pg_description::PgDescription, pg_enum::PgEnum, pg_index::PgIndex, pg_inherits::PgInherits,
    pg_namespace::PgNamespace, pg_operator::PgOperator, pg_proc::PgProc, pg_sequence::PgSequence,
    pg_type::PgType, SystemView,
};

/// One real `pg_attribute` row, as reported by the live server.
#[derive(Debug, Clone)]
struct RealColumn {
    name: String,
    attnum: i16,
    not_null: bool,
    /// `format_type(atttypid, NULL)` — the SQL spelling of the column's real
    /// type (`oid`, `name`, `"char"`, `smallint[]`, `oidvector`, ...).
    pg_type: String,
}

/// The Arrow types this crate is allowed to use to represent a given real
/// Postgres column type, or `None` if this harness has no mapping for that
/// type yet.
///
/// This encodes the representation convention the crate already follows
/// consistently across all 16 relations, rather than inventing a new one:
/// every OID-ish type is `UInt32`, every string-ish and single-byte-`"char"`
/// type is `Utf8` (Basin has no 1-byte char type, and `pg_node_tree` is an
/// opaque string), and every array/vector type is an Arrow `List` of the
/// element's mapping. `regproc` is an OID at the storage level and is
/// represented as one.
///
/// Returning `None` for an unrecognized type is deliberately NOT a pass: the
/// caller reports unmapped types as a harness gap, so a relation that starts
/// implementing a column of some type nobody thought about here cannot slip
/// through unchecked.
fn allowed_arrow_types(pg_type: &str) -> Option<Vec<DataType>> {
    let utf8 = || vec![DataType::Utf8];
    let list_of = |dt: DataType| vec![DataType::List(Arc::new(Field::new("item", dt, true)))];
    Some(match pg_type {
        "oid" | "regproc" | "xid" => vec![DataType::UInt32],
        "name" | "text" | "\"char\"" | "pg_node_tree" | "aclitem" => utf8(),
        "smallint" => vec![DataType::Int16],
        "integer" => vec![DataType::Int32],
        "bigint" => vec![DataType::Int64],
        "boolean" => vec![DataType::Boolean],
        "real" => vec![DataType::Float32],
        "double precision" => vec![DataType::Float64],
        "oidvector" | "oid[]" => list_of(DataType::UInt32),
        "int2vector" | "smallint[]" => list_of(DataType::Int16),
        "text[]" | "\"char\"[]" | "aclitem[]" | "name[]" => list_of(DataType::Utf8),
        _ => return None,
    })
}

/// Compare two Arrow types for the purpose above, ignoring a `List`'s child
/// *field* metadata (its name and nullability), which is arbitrary — only the
/// element type carries meaning here.
fn arrow_type_matches(declared: &DataType, allowed: &DataType) -> bool {
    match (declared, allowed) {
        (DataType::List(a), DataType::List(b)) => arrow_type_matches(a.data_type(), b.data_type()),
        (a, b) => a == b,
    }
}

/// The result of diffing one relation's declared [`arrow_schema`] against
/// its real, live-server column layout.
struct RelationReport {
    relation: String,
    /// Number of real columns (`attnum > 0`) the live server reports.
    real_total: usize,
    /// Number of this crate's declared fields matched by name to a real
    /// column.
    implemented: usize,
    /// `true` iff the matched real `attnum`s, taken in the order this
    /// crate's schema declares them, are strictly increasing — the
    /// order-preserving-subsequence property this harness exists to check.
    order_correct: bool,
    /// Declared fields whose name matches no real column at all.
    unknown_fields: Vec<String>,
    /// Declared fields whose nullability disagrees with the real
    /// `attnotnull`. `(field_name, field_is_nullable, real_not_null)`.
    nullability_mismatches: Vec<(String, bool, bool)>,
    /// Declared fields whose Arrow type is not an accepted representation of
    /// the real column's Postgres type.
    /// `(field_name, declared_arrow_type, real_pg_type, accepted_arrow_types)`.
    type_mismatches: Vec<(String, String, String, String)>,
    /// Real column types this harness has no Arrow mapping for. Reported as a
    /// failure (a gap in the check is not a pass) rather than skipped.
    /// `(field_name, real_pg_type)`.
    unmapped_types: Vec<(String, String)>,
}

impl RelationReport {
    fn ok(&self) -> bool {
        self.order_correct
            && self.unknown_fields.is_empty()
            && self.nullability_mismatches.is_empty()
            && self.type_mismatches.is_empty()
            && self.unmapped_types.is_empty()
    }
}

/// Diff one relation's declared schema against its real column layout.
fn diff_relation(relation: &str, schema: &SchemaRef, real: &[RealColumn]) -> RelationReport {
    let real_by_name: HashMap<&str, &RealColumn> =
        real.iter().map(|c| (c.name.as_str(), c)).collect();

    let mut unknown_fields = Vec::new();
    let mut nullability_mismatches = Vec::new();
    let mut type_mismatches = Vec::new();
    let mut unmapped_types = Vec::new();
    let mut matched_attnums: Vec<i16> = Vec::new();

    for field in schema.fields() {
        match real_by_name.get(field.name().as_str()) {
            None => unknown_fields.push(field.name().clone()),
            Some(real_col) => {
                matched_attnums.push(real_col.attnum);
                let expected_nullable = !real_col.not_null;
                if field.is_nullable() != expected_nullable {
                    nullability_mismatches.push((
                        field.name().clone(),
                        field.is_nullable(),
                        real_col.not_null,
                    ));
                }
                match allowed_arrow_types(&real_col.pg_type) {
                    None => unmapped_types.push((field.name().clone(), real_col.pg_type.clone())),
                    Some(allowed) => {
                        if !allowed
                            .iter()
                            .any(|a| arrow_type_matches(field.data_type(), a))
                        {
                            type_mismatches.push((
                                field.name().clone(),
                                format!("{:?}", field.data_type()),
                                real_col.pg_type.clone(),
                                format!("{allowed:?}"),
                            ));
                        }
                    }
                }
            }
        }
    }

    let order_correct = matched_attnums.windows(2).all(|w| w[0] < w[1]);

    RelationReport {
        relation: relation.to_string(),
        real_total: real.len(),
        implemented: matched_attnums.len(),
        order_correct,
        unknown_fields,
        nullability_mismatches,
        type_mismatches,
        unmapped_types,
    }
}

/// Query the live server for `relname`'s real column layout, in `attnum`
/// order — the same query the manual audits used (see `pg_index`'s module
/// docs).
async fn real_columns(client: &tokio_postgres::Client, relname: &str) -> Vec<RealColumn> {
    let query = format!(
        "SELECT attname, attnum, attnotnull, format_type(atttypid, NULL) FROM pg_attribute \
         WHERE attrelid = 'pg_catalog.{relname}'::regclass AND attnum > 0 AND NOT attisdropped \
         ORDER BY attnum"
    );
    let rows = client
        .query(&query, &[])
        .await
        .unwrap_or_else(|e| panic!("querying real layout of pg_catalog.{relname}: {e}"));
    rows.into_iter()
        .map(|row| RealColumn {
            name: row.get::<_, String>(0),
            attnum: row.get::<_, i16>(1),
            not_null: row.get::<_, bool>(2),
            pg_type: row.get::<_, String>(3),
        })
        .collect()
}

/// Diff [`PgType`]'s static row data — not just its column layout — against
/// the live server, one column at a time, for every OID it reports.
///
/// This is the content-level counterpart to the shape check above. `PgType`'s
/// `TYPES` table is hand-transcribed static data whose only prior
/// verification was a by-hand audit, and the single worst bug this project
/// has found in the type layer (`"char"`'s `typcategory` being read as `'S'`
/// rather than its real `'Z'`, which invented 37 casts Postgres does not
/// have) was exactly a wrong value in a hand-transcribed type-property table.
/// Rows are matched by OID; a row Basin reports for an OID the live server
/// does not have is a failure, as is any per-column disagreement.
async fn diff_pg_type_rows(client: &tokio_postgres::Client) -> Result<usize, Vec<String>> {
    let batch = PgType
        .scan(&MockCatalog::new(), &[])
        .expect("PgType::scan with no predicates");

    let col_u32 = |name: &str| -> Vec<u32> {
        let idx = batch.schema().index_of(name).expect("column exists");
        batch
            .column(idx)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap_or_else(|| panic!("{name} is UInt32"))
            .values()
            .to_vec()
    };
    let col_str = |name: &str| -> Vec<String> {
        let idx = batch.schema().index_of(name).expect("column exists");
        let a = batch
            .column(idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap_or_else(|| panic!("{name} is Utf8"));
        (0..a.len()).map(|i| a.value(i).to_string()).collect()
    };
    let oids = col_u32("oid");
    let typnamespaces = col_u32("typnamespace");
    let typelems = col_u32("typelem");
    let typarrays = col_u32("typarray");
    let typnames = col_str("typname");
    let typtypes = col_str("typtype");
    let typcategories = col_str("typcategory");
    let typlens = {
        let idx = batch.schema().index_of("typlen").expect("typlen exists");
        batch
            .column(idx)
            .as_any()
            .downcast_ref::<Int16Array>()
            .expect("typlen is Int16")
            .values()
            .to_vec()
    };

    let oid_list = oids
        .iter()
        .map(|o| o.to_string())
        .collect::<Vec<_>>()
        .join(",");
    let rows = client
        .query(
            &format!(
                "SELECT oid, typname, typnamespace, typlen, typtype::text, typcategory::text, \
                 typelem, typarray FROM pg_type WHERE oid IN ({oid_list})"
            ),
            &[],
        )
        .await
        .expect("querying live pg_type for every OID PgType reports");

    struct LiveType {
        typname: String,
        typnamespace: u32,
        typlen: i16,
        typtype: String,
        typcategory: String,
        typelem: u32,
        typarray: u32,
    }
    let live: HashMap<u32, LiveType> = rows
        .iter()
        .map(|r| {
            (
                r.get::<_, u32>(0),
                LiveType {
                    typname: r.get(1),
                    typnamespace: r.get(2),
                    typlen: r.get(3),
                    typtype: r.get(4),
                    typcategory: r.get(5),
                    typelem: r.get(6),
                    typarray: r.get(7),
                },
            )
        })
        .collect();

    let mut problems = Vec::new();
    for i in 0..oids.len() {
        let oid = oids[i];
        let Some(lv) = live.get(&oid) else {
            problems.push(format!(
                "pg_type oid {oid} ({}): Basin reports a row the live server has no pg_type \
                 entry for",
                typnames[i]
            ));
            continue;
        };
        let mut cmp = |col: &str, basin: String, real: String| {
            if basin != real {
                problems.push(format!(
                    "pg_type oid {oid} ({}): {col} Basin={basin} live={real}",
                    typnames[i]
                ));
            }
        };
        cmp("typname", typnames[i].clone(), lv.typname.clone());
        cmp(
            "typnamespace",
            typnamespaces[i].to_string(),
            lv.typnamespace.to_string(),
        );
        cmp("typlen", typlens[i].to_string(), lv.typlen.to_string());
        cmp("typtype", typtypes[i].clone(), lv.typtype.clone());
        cmp(
            "typcategory",
            typcategories[i].clone(),
            lv.typcategory.clone(),
        );
        cmp("typelem", typelems[i].to_string(), lv.typelem.to_string());
        cmp(
            "typarray",
            typarrays[i].to_string(),
            lv.typarray.to_string(),
        );
    }

    if problems.is_empty() {
        Ok(oids.len())
    } else {
        Err(problems)
    }
}

/// Every relation this crate registers, paired with its declared name (the
/// same string `SystemView::name()` and the real `pg_catalog.<name>` share).
/// Kept as a `(name, schema)` list built from each relation's own
/// `SystemView` impl rather than re-declaring names by hand, so this list
/// cannot itself drift from what the crate actually exposes.
fn relations() -> Vec<(String, SchemaRef)> {
    let views: Vec<Box<dyn SystemView>> = vec![
        Box::new(PgAm),
        Box::new(PgAttrDef),
        Box::new(PgAttribute),
        Box::new(PgCast),
        Box::new(PgClass),
        Box::new(PgConstraint),
        Box::new(PgDepend),
        Box::new(PgDescription),
        Box::new(PgEnum),
        Box::new(PgIndex),
        Box::new(PgInherits),
        Box::new(PgNamespace),
        Box::new(PgOperator),
        Box::new(PgProc),
        Box::new(PgSequence),
        Box::new(PgType),
    ];
    views
        .into_iter()
        .map(|v| (v.name().to_string(), v.schema()))
        .collect()
}

fn print_banner(msg: &str) {
    println!("=== catalog_fidelity: {msg} ===");
}

async fn run() -> Result<(), String> {
    let dsn = match std::env::var("PG_DIFF_TEST_DSN") {
        Ok(v) if !v.trim().is_empty() => v,
        _ => {
            print_banner(
                "SKIPPED — PG_DIFF_TEST_DSN is unset. 0 of the crate's relations were checked \
                 against a live server this run. Set PG_DIFF_TEST_DSN to a libpq DSN (e.g. \
                 postgres://postgres:postgres@127.0.0.1:5432/postgres) to actually run this check.",
            );
            return Ok(());
        }
    };

    let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
        .await
        .map_err(|e| format!("PG_DIFF_TEST_DSN connect failed: {e}"))?;
    tokio::spawn(async move {
        let _ = connection.await;
    });

    print_banner("running against live PostgreSQL (PG_DIFF_TEST_DSN set)");

    let relations = relations();
    let mut reports = Vec::with_capacity(relations.len());
    for (name, schema) in &relations {
        let real = real_columns(&client, name).await;
        reports.push(diff_relation(name, schema, &real));
    }

    // ---- metric: per-relation summary, always printed ----------------
    let name_width = reports.iter().map(|r| r.relation.len()).max().unwrap_or(0);
    let flag = |ok: bool| if ok { "OK" } else { "**BROKEN**" };
    println!(
        "{:<width$}  {:>17}  {:<11}  {:<12}  {:<11}",
        "relation",
        "implemented/real",
        "order",
        "nullability",
        "types",
        width = name_width
    );
    for r in &reports {
        println!(
            "{:<width$}  {:>8}/{:<8}  {:<11}  {:<12}  {:<11}",
            r.relation,
            r.implemented,
            r.real_total,
            flag(r.order_correct),
            flag(r.nullability_mismatches.is_empty()),
            flag(r.type_mismatches.is_empty() && r.unmapped_types.is_empty()),
            width = name_width
        );
    }

    // ---- content check: pg_type's static rows, cell by cell -----------
    let pg_type_rows = match diff_pg_type_rows(&client).await {
        Ok(n) => {
            println!(
                "pg_type row content: OK — {n} rows x 7 columns \
                 (typname/typnamespace/typlen/typtype/typcategory/typelem/typarray) \
                 verified against live pg_type"
            );
            n
        }
        Err(problems) => {
            println!("pg_type row content: **BROKEN** — {} cells", problems.len());
            return Err(format!(
                "pg_type row content disagrees with live PostgreSQL in {} places:\n  {}",
                problems.len(),
                problems.join("\n  ")
            ));
        }
    };

    let broken: Vec<&RelationReport> = reports.iter().filter(|r| !r.ok()).collect();
    if broken.is_empty() {
        print_banner(&format!(
            "PASS — {} relations / {} implemented columns checked, all order-correct, no \
             unknown columns, no nullability mismatches, no type mismatches; plus {} pg_type \
             rows verified cell-by-cell",
            reports.len(),
            reports.iter().map(|r| r.implemented).sum::<usize>(),
            pg_type_rows,
        ));
        return Ok(());
    }

    let mut detail = String::new();
    for r in &broken {
        detail.push_str(&format!("\n  {}:", r.relation));
        if !r.order_correct {
            detail.push_str("\n    - implemented columns are NOT in real attnum order");
        }
        if !r.unknown_fields.is_empty() {
            detail.push_str(&format!(
                "\n    - fields with no matching real column: {:?}",
                r.unknown_fields
            ));
        }
        for (field, is_nullable, real_not_null) in &r.nullability_mismatches {
            detail.push_str(&format!(
                "\n    - {field}: schema says is_nullable={is_nullable}, real attnotnull={real_not_null}"
            ));
        }
        for (field, declared, real_ty, allowed) in &r.type_mismatches {
            detail.push_str(&format!(
                "\n    - {field}: declared Arrow {declared}, but the real column is \
                 Postgres `{real_ty}` (accepted: {allowed})"
            ));
        }
        for (field, real_ty) in &r.unmapped_types {
            detail.push_str(&format!(
                "\n    - {field}: this harness has no Arrow mapping for Postgres `{real_ty}`, \
                 so its type went UNCHECKED — extend allowed_arrow_types()"
            ));
        }
    }
    Err(format!(
        "{} of {} relations failed catalog fidelity:{}",
        broken.len(),
        reports.len(),
        detail
    ))
}

fn main() {
    // No libtest here (see module docs on `harness = false`): build a
    // minimal runtime by hand rather than depending on `#[tokio::main]`
    // pulling in more than this crate's dev-dependencies declare.
    let rt = tokio::runtime::Runtime::new().expect("building a tokio runtime");
    match rt.block_on(run()) {
        Ok(()) => {}
        Err(msg) => {
            eprintln!("{msg}");
            std::process::exit(1);
        }
    }
}
