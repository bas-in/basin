//! Exhaustive differential matrix: Basin's cast table, operator table, and
//! OID table, checked cell-by-cell against a LIVE PostgreSQL.
//!
//! # Why this exists
//!
//! An ad-hoc, by-hand mechanical diff of Basin's cast table against a live
//! PostgreSQL — all 552 ordered pairs among the 24 types this crate
//! represents — found that [`basin_pgtype::cast::is_string_category`] used to
//! wrongly count the single-byte `"char"` type (oid 18, real `typcategory`
//! `'Z'`) as string-category, which INVENTED 37 casts Postgres does not have.
//! That check was run once, by hand. This file is what makes it permanent.
//!
//! Three tables, three live oracles:
//!
//! 1. **Casts** — every ordered pair among the 24 types Basin represents,
//!    compared against live `pg_cast` plus Postgres's own I/O-conversion
//!    fallback (reproduced here independently from live `pg_type.typcategory`,
//!    NOT by calling Basin's own `is_string_category` — an oracle that calls
//!    the code under test isn't an oracle).
//! 2. **Operators** — every row in [`basin_pgtype::operator::OPERATORS`],
//!    checked as a full tuple (oid, name, left, right, result) against live
//!    `pg_operator`, because operand ORDER is where the errors live (`int4 =
//!    int8` is oid 15; `int8 = int4` is oid 416 — a name-only check would
//!    pass a table with every asymmetric pair reversed).
//! 3. **Types** — every OID `basin_pgtype::oid` exports, checked against live
//!    `pg_type` for `typname`/`typlen`/`typtype`/`typcategory`, plus the
//!    array pairing (`typarray`) cross-checked by OID identity — Basin spells
//!    array types `X_ARRAY` where Postgres spells them `_x`, so a name
//!    comparison there would be a false-positive machine.
//!
//! # Skipping
//!
//! Every test here requires `PG_DIFF_TEST_DSN` (same env var and DSN
//! convention as `crates/basin-engine/tests/differential_pg.rs` and
//! `tests/integration/tests/differential_pg.rs`). Without it, each test
//! prints a `SKIPPED` banner to stderr that states the table name and the
//! total pair/row count that was NOT compared, then returns — a skip is
//! never allowed to be silently indistinguishable from "compared everything
//! and found zero problems". `scripts/check-differential.sh`'s own header
//! explains why that distinction matters: this suite once sat un-run in CI
//! for its whole existence because a bare early-return "looked like" ok.
//!
//! Run against a live server:
//!
//! ```sh
//! PG_DIFF_TEST_DSN='postgres://postgres:postgres@127.0.0.1:5432/postgres' \
//!   cargo test -p basin-pgtype --test type_matrix
//! ```
//!
//! # This test is allowed to fail
//!
//! It intentionally has NO baseline/allowlist mechanism, unlike
//! `differential_pg`'s `check-differential.sh` gate. If Basin's cast,
//! operator, or type tables disagree with a live PostgreSQL, this test fails
//! and prints every disagreement — that is the correct outcome, not a bug in
//! the harness. Weakening this test to make it pass defeats the entire
//! reason it exists.

use std::collections::HashMap;

use basin_pgtype::cast::{cast_kind, CastKind};
use basin_pgtype::operator::OPERATORS;
use basin_pgtype::{oid, Oid};

const DSN_VAR: &str = "PG_DIFF_TEST_DSN";

/// Connect using the repo's standard differential-test DSN convention.
/// Returns `None` (never panics) if the var is unset/empty or the server is
/// unreachable — callers are responsible for turning that into a loud,
/// metrics-shaped skip rather than a quiet one.
async fn connect() -> Option<tokio_postgres::Client> {
    let dsn = match std::env::var(DSN_VAR) {
        Ok(v) if !v.trim().is_empty() => v,
        _ => return None,
    };
    match tokio_postgres::connect(&dsn, tokio_postgres::NoTls).await {
        Ok((client, connection)) => {
            tokio::spawn(async move {
                if let Err(e) = connection.await {
                    eprintln!("[type_matrix] connection driver error: {e}");
                }
            });
            Some(client)
        }
        Err(e) => {
            eprintln!("[type_matrix] could not connect to {DSN_VAR}: {e}");
            None
        }
    }
}

/// Print the unmissable "we compared nothing" banner. Every table's skip
/// path goes through this one function so the shape is identical everywhere:
/// table name, env var, and the total this run did NOT verify.
fn print_skip_banner(table: &str, total: usize, unit: &str) {
    eprintln!(
        "[type_matrix] {table} — SKIPPED: {DSN_VAR} is unset or the server is unreachable. \
         0 of {total} {unit} compared. THIS IS NOT A VERIFIED PASS — it is the absence of a check."
    );
}

// ============================================================================
// Table 1 — Casts
// ============================================================================

/// The 24 types this crate represents (see `basin_pgtype::oid` module docs
/// and `crate::cast`'s own scope). This list — and therefore 24×23 = 552
/// ordered pairs — is exactly the set the original hand-run mechanical diff
/// covered.
const CAST_TYPES: &[(&str, Oid)] = &[
    ("bool", oid::BOOL),
    ("bytea", oid::BYTEA),
    ("char", oid::CHAR),
    ("name", oid::NAME),
    ("int8", oid::INT8),
    ("int2", oid::INT2),
    ("int4", oid::INT4),
    ("text", oid::TEXT),
    ("oid", oid::OID),
    ("json", oid::JSON),
    ("xml", oid::XML),
    ("float4", oid::FLOAT4),
    ("float8", oid::FLOAT8),
    ("bpchar", oid::BPCHAR),
    ("varchar", oid::VARCHAR),
    ("date", oid::DATE),
    ("time", oid::TIME),
    ("timestamp", oid::TIMESTAMP),
    ("timestamptz", oid::TIMESTAMPTZ),
    ("interval", oid::INTERVAL),
    ("timetz", oid::TIMETZ),
    ("numeric", oid::NUMERIC),
    ("uuid", oid::UUID),
    ("jsonb", oid::JSONB),
];

#[tokio::test]
async fn cast_matrix_matches_live_postgres() {
    assert_eq!(CAST_TYPES.len(), 24, "the 24-type universe changed size");
    let total_pairs = CAST_TYPES.len() * (CAST_TYPES.len() - 1);
    assert_eq!(total_pairs, 552, "24 x 23 ordered pairs");

    let Some(client) = connect().await else {
        print_skip_banner("CASTS", total_pairs, "pairs");
        return;
    };

    let oid_list = CAST_TYPES
        .iter()
        .map(|(_, o)| o.get().to_string())
        .collect::<Vec<_>>()
        .join(",");

    // Ground truth input #1: typcategory, straight from pg_type — NOT from
    // Basin's `is_string_category`. This is what makes the oracle
    // independent of the code under test.
    let cat_rows = client
        .query(
            &format!("SELECT oid, typcategory::text FROM pg_type WHERE oid IN ({oid_list})"),
            &[],
        )
        .await
        .expect("query pg_type.typcategory for the 24-type universe");
    let mut category: HashMap<u32, char> = HashMap::new();
    for row in &cat_rows {
        let o: u32 = row.get(0);
        let c: String = row.get(1);
        category.insert(o, c.chars().next().expect("typcategory is one char"));
    }
    assert_eq!(
        category.len(),
        CAST_TYPES.len(),
        "expected exactly {} live pg_type rows for the 24-type universe — an OID in CAST_TYPES \
         does not exist on the server, or the server merged two of them",
        CAST_TYPES.len()
    );

    // Ground truth input #2: every real pg_cast row among the 24 types.
    let cast_rows = client
        .query(
            &format!(
                "SELECT castsource, casttarget, castcontext::text FROM pg_cast \
                 WHERE castsource IN ({oid_list}) AND casttarget IN ({oid_list})"
            ),
            &[],
        )
        .await
        .expect("query pg_cast for the 24-type universe");
    let mut pg_cast: HashMap<(u32, u32), char> = HashMap::new();
    for row in &cast_rows {
        let s: u32 = row.get(0);
        let t: u32 = row.get(1);
        let c: String = row.get(2);
        pg_cast.insert((s, t), c.chars().next().expect("castcontext is one char"));
    }

    // The oracle: Postgres's own `find_coercion_pathway` ordering, exactly as
    // documented in `basin_pgtype::cast`'s module docs — a real pg_cast row
    // wins; failing that, the I/O fallback fires on typcategory = 'S'
    // (assignment into a string type; explicit out of one). Reproduced here
    // from LIVE data, independently of Basin's own implementation of the same
    // rule.
    let oracle = |from: u32, to: u32| -> Option<CastKind> {
        if let Some(&ctx) = pg_cast.get(&(from, to)) {
            return CastKind::from_char(ctx);
        }
        if category[&to] == 'S' {
            return Some(CastKind::Assignment);
        }
        if category[&from] == 'S' {
            return Some(CastKind::Explicit);
        }
        None
    };

    let mut compared = 0usize;
    let mut dangerous = 0usize; // Basin allows a cast Postgres does not.
    let mut missing = 0usize; // Postgres allows a cast Basin does not.
    let mut context_mismatch = 0usize; // Both allow it, at different strictness.
    let mut disagreements: Vec<String> = Vec::new();

    for &(from_name, from_oid) in CAST_TYPES {
        for &(to_name, to_oid) in CAST_TYPES {
            if from_oid == to_oid {
                continue;
            }
            compared += 1;

            let oracle_kind = oracle(from_oid.get(), to_oid.get());
            let basin_kind = cast_kind(from_oid, to_oid);

            if oracle_kind == basin_kind {
                continue;
            }
            match (basin_kind, oracle_kind) {
                (Some(bk), None) => {
                    dangerous += 1;
                    disagreements.push(format!(
                        "DANGEROUS  {from_name:12} -> {to_name:12}  Basin allows {bk:?}, \
                         Postgres has NO cast at all — Basin would accept SQL Postgres rejects"
                    ));
                }
                (None, Some(ok)) => {
                    missing += 1;
                    disagreements.push(format!(
                        "MISSING    {from_name:12} -> {to_name:12}  Postgres allows {ok:?}, \
                         Basin has NO cast"
                    ));
                }
                (Some(bk), Some(ok)) => {
                    context_mismatch += 1;
                    disagreements.push(format!(
                        "CONTEXT    {from_name:12} -> {to_name:12}  Basin says {bk:?}, \
                         Postgres says {ok:?}"
                    ));
                }
                (None, None) => unreachable!("equal Nones can't reach the mismatch branch"),
            }
        }
    }

    eprintln!(
        "[type_matrix] CASTS — {compared} of {total_pairs} ordered pairs compared across {} types \
         · {} disagreements ({dangerous} dangerous [Basin accepts what Postgres rejects], \
         {missing} missing [Postgres accepts what Basin rejects], {context_mismatch} context mismatches)",
        CAST_TYPES.len(),
        disagreements.len(),
    );
    for d in &disagreements {
        eprintln!("  {d}");
    }

    assert!(
        disagreements.is_empty(),
        "{} of {total_pairs} cast pairs disagree with live PostgreSQL:\n{}",
        disagreements.len(),
        disagreements.join("\n")
    );
}

// ============================================================================
// Table 2 — Operators
// ============================================================================

/// Operators Postgres declares on a polymorphic pseudo-type (`anyarray` /
/// `anycompatiblearray`), which `basin_pgtype::operator`'s module docs
/// monomorphize at a handful of concrete element types sharing the ONE real
/// oid (confirmed live: `anyarray` is oid 2277, `anycompatiblearray` is oid
/// 5078). Their left/right/result legitimately differ per concrete row while
/// the live catalog reports the pseudo-type once, so comparing those fields
/// directly against the live row would be a false-positive machine, not a
/// check. These get a name-only tuple check, plus an explicit assertion that
/// the live row really is still the documented pseudo-type — i.e. that
/// monomorphizing was still warranted, not a stale assumption.
const POLYMORPHIC_ANYARRAY_OIDS: &[u32] = &[2751, 2752, 2750]; // @>, <@, &&
const POLYMORPHIC_ANYCOMPATIBLEARRAY_OIDS: &[u32] = &[375]; // ||
const ANYCOMPATIBLEARRAY_OID: u32 = 5078;

#[tokio::test]
async fn operator_table_matches_live_postgres() {
    let total_rows = OPERATORS.len();

    let Some(client) = connect().await else {
        print_skip_banner("OPERATORS", total_rows, "rows");
        return;
    };

    let mut unique_oids: Vec<u32> = OPERATORS.iter().map(|op| op.oid.get()).collect();
    unique_oids.sort_unstable();
    unique_oids.dedup();
    let oid_list = unique_oids
        .iter()
        .map(|o| o.to_string())
        .collect::<Vec<_>>()
        .join(",");

    let rows = client
        .query(
            &format!(
                "SELECT oid, oprname, oprleft, oprright, oprresult FROM pg_operator \
                 WHERE oid IN ({oid_list})"
            ),
            &[],
        )
        .await
        .expect("query pg_operator");

    struct LiveOp {
        name: String,
        left: u32,
        right: u32,
        result: u32,
    }
    let mut live: HashMap<u32, LiveOp> = HashMap::new();
    for row in &rows {
        let oid_val: u32 = row.get(0);
        live.insert(
            oid_val,
            LiveOp {
                name: row.get(1),
                left: row.get(2),
                right: row.get(3),
                result: row.get(4),
            },
        );
    }

    let mut verified = 0usize;
    let mut polymorphic_checked = 0usize;
    let mut disagreements: Vec<String> = Vec::new();

    for op in OPERATORS {
        verified += 1;
        let oid_val = op.oid.get();

        let Some(lv) = live.get(&oid_val) else {
            disagreements.push(format!(
                "oid {oid_val}: no live pg_operator row at all for Basin's {:?} row \
                 (name={:?}, left={:?}, right={})",
                op.oid, op.name, op.left, op.right
            ));
            continue;
        };

        if POLYMORPHIC_ANYARRAY_OIDS.contains(&oid_val)
            || POLYMORPHIC_ANYCOMPATIBLEARRAY_OIDS.contains(&oid_val)
        {
            polymorphic_checked += 1;
            if lv.name != op.name {
                disagreements.push(format!(
                    "oid {oid_val}: name mismatch: Basin={:?} live={:?}",
                    op.name, lv.name
                ));
            }
            let expected_pseudo = if POLYMORPHIC_ANYARRAY_OIDS.contains(&oid_val) {
                oid::ANYARRAY.get()
            } else {
                ANYCOMPATIBLEARRAY_OID
            };
            if lv.left != expected_pseudo || lv.right != expected_pseudo {
                disagreements.push(format!(
                    "oid {oid_val} ({}): expected the live row to still be declared on the \
                     polymorphic pseudo-type {expected_pseudo}, but live left={}, right={} — \
                     monomorphizing this oid at concrete element types may no longer be warranted",
                    op.name, lv.left, lv.right
                ));
            }
            continue;
        }

        let basin_left = op.left.map(|o| o.get()).unwrap_or(0);
        let basin_right = op.right.get();
        let basin_result = op.result.get();
        if op.name != lv.name
            || basin_left != lv.left
            || basin_right != lv.right
            || basin_result != lv.result
        {
            disagreements.push(format!(
                "oid {oid_val}: Basin=(name={:?}, left={:?}, right={}, result={}) \
                 live=(name={:?}, left={}, right={}, result={})",
                op.name, op.left, basin_right, basin_result, lv.name, lv.left, lv.right, lv.result
            ));
        }
    }

    eprintln!(
        "[type_matrix] OPERATORS — {verified} of {total_rows} rows verified as full tuples \
         (oid, name, left, right, result) against live pg_operator \
         ({} distinct oids queried, {polymorphic_checked} polymorphic-array rows checked \
         name-only) · {} disagreements",
        unique_oids.len(),
        disagreements.len(),
    );
    for d in &disagreements {
        eprintln!("  {d}");
    }

    assert!(
        disagreements.is_empty(),
        "{} of {total_rows} operator rows disagree with live pg_operator:\n{}",
        disagreements.len(),
        disagreements.join("\n")
    );
}

// ============================================================================
// Table 3 — Types
// ============================================================================

/// One fact per OID `basin_pgtype::oid` exports (excluding [`oid::Oid::INVALID`],
/// which is not a real type). Values below were pulled from a live PostgreSQL
/// 18.2 (`SELECT oid, typname, typlen, typtype, typcategory, typarray FROM
/// pg_type WHERE oid = ANY(...)`), not recalled from memory — this test
/// re-verifies every field against whatever server `PG_DIFF_TEST_DSN` points
/// at, so drift on a different major version is caught rather than assumed
/// away. `typarray` is 0 where the type has no builtin array type.
struct TypeFact {
    label: &'static str,
    oid: Oid,
    typname: &'static str,
    typlen: i16,
    typtype: char,
    typcategory: char,
    typarray: u32,
}

const TYPE_FACTS: &[TypeFact] = &[
    // ─── Scalar builtins (24) ───────────────────────────────────────────────
    TypeFact {
        label: "BOOL",
        oid: oid::BOOL,
        typname: "bool",
        typlen: 1,
        typtype: 'b',
        typcategory: 'B',
        typarray: 1000,
    },
    TypeFact {
        label: "BYTEA",
        oid: oid::BYTEA,
        typname: "bytea",
        typlen: -1,
        typtype: 'b',
        typcategory: 'U',
        typarray: 1001,
    },
    TypeFact {
        label: "CHAR",
        oid: oid::CHAR,
        typname: "char",
        typlen: 1,
        typtype: 'b',
        typcategory: 'Z',
        typarray: 1002,
    },
    TypeFact {
        label: "NAME",
        oid: oid::NAME,
        typname: "name",
        typlen: 64,
        typtype: 'b',
        typcategory: 'S',
        typarray: 1003,
    },
    TypeFact {
        label: "INT8",
        oid: oid::INT8,
        typname: "int8",
        typlen: 8,
        typtype: 'b',
        typcategory: 'N',
        typarray: 1016,
    },
    TypeFact {
        label: "INT2",
        oid: oid::INT2,
        typname: "int2",
        typlen: 2,
        typtype: 'b',
        typcategory: 'N',
        typarray: 1005,
    },
    TypeFact {
        label: "INT4",
        oid: oid::INT4,
        typname: "int4",
        typlen: 4,
        typtype: 'b',
        typcategory: 'N',
        typarray: 1007,
    },
    TypeFact {
        label: "TEXT",
        oid: oid::TEXT,
        typname: "text",
        typlen: -1,
        typtype: 'b',
        typcategory: 'S',
        typarray: 1009,
    },
    TypeFact {
        label: "OID",
        oid: oid::OID,
        typname: "oid",
        typlen: 4,
        typtype: 'b',
        typcategory: 'N',
        typarray: 1028,
    },
    TypeFact {
        label: "JSON",
        oid: oid::JSON,
        typname: "json",
        typlen: -1,
        typtype: 'b',
        typcategory: 'U',
        typarray: 199,
    },
    TypeFact {
        label: "XML",
        oid: oid::XML,
        typname: "xml",
        typlen: -1,
        typtype: 'b',
        typcategory: 'U',
        typarray: 143,
    },
    TypeFact {
        label: "FLOAT4",
        oid: oid::FLOAT4,
        typname: "float4",
        typlen: 4,
        typtype: 'b',
        typcategory: 'N',
        typarray: 1021,
    },
    TypeFact {
        label: "FLOAT8",
        oid: oid::FLOAT8,
        typname: "float8",
        typlen: 8,
        typtype: 'b',
        typcategory: 'N',
        typarray: 1022,
    },
    TypeFact {
        label: "BPCHAR",
        oid: oid::BPCHAR,
        typname: "bpchar",
        typlen: -1,
        typtype: 'b',
        typcategory: 'S',
        typarray: 1014,
    },
    TypeFact {
        label: "VARCHAR",
        oid: oid::VARCHAR,
        typname: "varchar",
        typlen: -1,
        typtype: 'b',
        typcategory: 'S',
        typarray: 1015,
    },
    TypeFact {
        label: "DATE",
        oid: oid::DATE,
        typname: "date",
        typlen: 4,
        typtype: 'b',
        typcategory: 'D',
        typarray: 1182,
    },
    TypeFact {
        label: "TIME",
        oid: oid::TIME,
        typname: "time",
        typlen: 8,
        typtype: 'b',
        typcategory: 'D',
        typarray: 1183,
    },
    TypeFact {
        label: "TIMESTAMP",
        oid: oid::TIMESTAMP,
        typname: "timestamp",
        typlen: 8,
        typtype: 'b',
        typcategory: 'D',
        typarray: 1115,
    },
    TypeFact {
        label: "TIMESTAMPTZ",
        oid: oid::TIMESTAMPTZ,
        typname: "timestamptz",
        typlen: 8,
        typtype: 'b',
        typcategory: 'D',
        typarray: 1185,
    },
    TypeFact {
        label: "INTERVAL",
        oid: oid::INTERVAL,
        typname: "interval",
        typlen: 16,
        typtype: 'b',
        typcategory: 'T',
        typarray: 1187,
    },
    TypeFact {
        label: "TIMETZ",
        oid: oid::TIMETZ,
        typname: "timetz",
        typlen: 12,
        typtype: 'b',
        typcategory: 'D',
        typarray: 1270,
    },
    TypeFact {
        label: "NUMERIC",
        oid: oid::NUMERIC,
        typname: "numeric",
        typlen: -1,
        typtype: 'b',
        typcategory: 'N',
        typarray: 1231,
    },
    TypeFact {
        label: "UUID",
        oid: oid::UUID,
        typname: "uuid",
        typlen: 16,
        typtype: 'b',
        typcategory: 'U',
        typarray: 2951,
    },
    TypeFact {
        label: "JSONB",
        oid: oid::JSONB,
        typname: "jsonb",
        typlen: -1,
        typtype: 'b',
        typcategory: 'U',
        typarray: 3807,
    },
    // ─── Pseudo-types (3) ───────────────────────────────────────────────────
    TypeFact {
        label: "UNKNOWN",
        oid: oid::UNKNOWN,
        typname: "unknown",
        typlen: -2,
        typtype: 'p',
        typcategory: 'X',
        typarray: 0,
    },
    TypeFact {
        label: "VOID",
        oid: oid::VOID,
        typname: "void",
        typlen: 4,
        typtype: 'p',
        typcategory: 'P',
        typarray: 0,
    },
    TypeFact {
        label: "RECORD",
        oid: oid::RECORD,
        typname: "record",
        typlen: -1,
        typtype: 'p',
        typcategory: 'P',
        typarray: 2287,
    },
    // ─── Catalog-internal builtins (4) ──────────────────────────────────────
    TypeFact {
        label: "OIDVECTOR",
        oid: oid::OIDVECTOR,
        typname: "oidvector",
        typlen: -1,
        typtype: 'b',
        typcategory: 'A',
        typarray: 1013,
    },
    TypeFact {
        label: "INT2VECTOR",
        oid: oid::INT2VECTOR,
        typname: "int2vector",
        typlen: -1,
        typtype: 'b',
        typcategory: 'A',
        typarray: 1006,
    },
    TypeFact {
        label: "PG_NODE_TREE",
        oid: oid::PG_NODE_TREE,
        typname: "pg_node_tree",
        typlen: -1,
        typtype: 'b',
        typcategory: 'Z',
        typarray: 0,
    },
    TypeFact {
        label: "ANYARRAY",
        oid: oid::ANYARRAY,
        typname: "anyarray",
        typlen: -1,
        typtype: 'p',
        typcategory: 'P',
        typarray: 0,
    },
    // ─── Range builtins (6) ─────────────────────────────────────────────────
    TypeFact {
        label: "INT4RANGE",
        oid: oid::INT4RANGE,
        typname: "int4range",
        typlen: -1,
        typtype: 'r',
        typcategory: 'R',
        typarray: 3905,
    },
    TypeFact {
        label: "NUMRANGE",
        oid: oid::NUMRANGE,
        typname: "numrange",
        typlen: -1,
        typtype: 'r',
        typcategory: 'R',
        typarray: 3907,
    },
    TypeFact {
        label: "TSRANGE",
        oid: oid::TSRANGE,
        typname: "tsrange",
        typlen: -1,
        typtype: 'r',
        typcategory: 'R',
        typarray: 3909,
    },
    TypeFact {
        label: "TSTZRANGE",
        oid: oid::TSTZRANGE,
        typname: "tstzrange",
        typlen: -1,
        typtype: 'r',
        typcategory: 'R',
        typarray: 3911,
    },
    TypeFact {
        label: "DATERANGE",
        oid: oid::DATERANGE,
        typname: "daterange",
        typlen: -1,
        typtype: 'r',
        typcategory: 'R',
        typarray: 3913,
    },
    TypeFact {
        label: "INT8RANGE",
        oid: oid::INT8RANGE,
        typname: "int8range",
        typlen: -1,
        typtype: 'r',
        typcategory: 'R',
        typarray: 3927,
    },
    // ─── Array builtins (22 scalar arrays) ──────────────────────────────────
    // All arrays: typlen -1, typtype 'b', typcategory 'A', typarray 0 (arrays
    // do not nest — see `oid::arrays_do_not_nest`). typname is Postgres's
    // `_x` spelling, NOT Basin's `X_ARRAY` — do not "fix" these to match the
    // Rust identifier.
    TypeFact {
        label: "BOOL_ARRAY",
        oid: oid::BOOL_ARRAY,
        typname: "_bool",
        typlen: -1,
        typtype: 'b',
        typcategory: 'A',
        typarray: 0,
    },
    TypeFact {
        label: "BYTEA_ARRAY",
        oid: oid::BYTEA_ARRAY,
        typname: "_bytea",
        typlen: -1,
        typtype: 'b',
        typcategory: 'A',
        typarray: 0,
    },
    TypeFact {
        label: "CHAR_ARRAY",
        oid: oid::CHAR_ARRAY,
        typname: "_char",
        typlen: -1,
        typtype: 'b',
        typcategory: 'A',
        typarray: 0,
    },
    TypeFact {
        label: "NAME_ARRAY",
        oid: oid::NAME_ARRAY,
        typname: "_name",
        typlen: -1,
        typtype: 'b',
        typcategory: 'A',
        typarray: 0,
    },
    TypeFact {
        label: "INT2_ARRAY",
        oid: oid::INT2_ARRAY,
        typname: "_int2",
        typlen: -1,
        typtype: 'b',
        typcategory: 'A',
        typarray: 0,
    },
    TypeFact {
        label: "INT4_ARRAY",
        oid: oid::INT4_ARRAY,
        typname: "_int4",
        typlen: -1,
        typtype: 'b',
        typcategory: 'A',
        typarray: 0,
    },
    TypeFact {
        label: "TEXT_ARRAY",
        oid: oid::TEXT_ARRAY,
        typname: "_text",
        typlen: -1,
        typtype: 'b',
        typcategory: 'A',
        typarray: 0,
    },
    TypeFact {
        label: "BPCHAR_ARRAY",
        oid: oid::BPCHAR_ARRAY,
        typname: "_bpchar",
        typlen: -1,
        typtype: 'b',
        typcategory: 'A',
        typarray: 0,
    },
    TypeFact {
        label: "VARCHAR_ARRAY",
        oid: oid::VARCHAR_ARRAY,
        typname: "_varchar",
        typlen: -1,
        typtype: 'b',
        typcategory: 'A',
        typarray: 0,
    },
    TypeFact {
        label: "INT8_ARRAY",
        oid: oid::INT8_ARRAY,
        typname: "_int8",
        typlen: -1,
        typtype: 'b',
        typcategory: 'A',
        typarray: 0,
    },
    TypeFact {
        label: "FLOAT4_ARRAY",
        oid: oid::FLOAT4_ARRAY,
        typname: "_float4",
        typlen: -1,
        typtype: 'b',
        typcategory: 'A',
        typarray: 0,
    },
    TypeFact {
        label: "FLOAT8_ARRAY",
        oid: oid::FLOAT8_ARRAY,
        typname: "_float8",
        typlen: -1,
        typtype: 'b',
        typcategory: 'A',
        typarray: 0,
    },
    TypeFact {
        label: "OID_ARRAY",
        oid: oid::OID_ARRAY,
        typname: "_oid",
        typlen: -1,
        typtype: 'b',
        typcategory: 'A',
        typarray: 0,
    },
    TypeFact {
        label: "DATE_ARRAY",
        oid: oid::DATE_ARRAY,
        typname: "_date",
        typlen: -1,
        typtype: 'b',
        typcategory: 'A',
        typarray: 0,
    },
    TypeFact {
        label: "TIME_ARRAY",
        oid: oid::TIME_ARRAY,
        typname: "_time",
        typlen: -1,
        typtype: 'b',
        typcategory: 'A',
        typarray: 0,
    },
    TypeFact {
        label: "TIMESTAMP_ARRAY",
        oid: oid::TIMESTAMP_ARRAY,
        typname: "_timestamp",
        typlen: -1,
        typtype: 'b',
        typcategory: 'A',
        typarray: 0,
    },
    TypeFact {
        label: "TIMESTAMPTZ_ARRAY",
        oid: oid::TIMESTAMPTZ_ARRAY,
        typname: "_timestamptz",
        typlen: -1,
        typtype: 'b',
        typcategory: 'A',
        typarray: 0,
    },
    TypeFact {
        label: "INTERVAL_ARRAY",
        oid: oid::INTERVAL_ARRAY,
        typname: "_interval",
        typlen: -1,
        typtype: 'b',
        typcategory: 'A',
        typarray: 0,
    },
    TypeFact {
        label: "NUMERIC_ARRAY",
        oid: oid::NUMERIC_ARRAY,
        typname: "_numeric",
        typlen: -1,
        typtype: 'b',
        typcategory: 'A',
        typarray: 0,
    },
    TypeFact {
        label: "JSON_ARRAY",
        oid: oid::JSON_ARRAY,
        typname: "_json",
        typlen: -1,
        typtype: 'b',
        typcategory: 'A',
        typarray: 0,
    },
    TypeFact {
        label: "JSONB_ARRAY",
        oid: oid::JSONB_ARRAY,
        typname: "_jsonb",
        typlen: -1,
        typtype: 'b',
        typcategory: 'A',
        typarray: 0,
    },
    TypeFact {
        label: "UUID_ARRAY",
        oid: oid::UUID_ARRAY,
        typname: "_uuid",
        typlen: -1,
        typtype: 'b',
        typcategory: 'A',
        typarray: 0,
    },
    // ─── Vector array builtins (2) ──────────────────────────────────────────
    TypeFact {
        label: "OIDVECTOR_ARRAY",
        oid: oid::OIDVECTOR_ARRAY,
        typname: "_oidvector",
        typlen: -1,
        typtype: 'b',
        typcategory: 'A',
        typarray: 0,
    },
    TypeFact {
        label: "INT2VECTOR_ARRAY",
        oid: oid::INT2VECTOR_ARRAY,
        typname: "_int2vector",
        typlen: -1,
        typtype: 'b',
        typcategory: 'A',
        typarray: 0,
    },
    // ─── Range array builtins (6) ────────────────────────────────────────────
    TypeFact {
        label: "INT4RANGE_ARRAY",
        oid: oid::INT4RANGE_ARRAY,
        typname: "_int4range",
        typlen: -1,
        typtype: 'b',
        typcategory: 'A',
        typarray: 0,
    },
    TypeFact {
        label: "NUMRANGE_ARRAY",
        oid: oid::NUMRANGE_ARRAY,
        typname: "_numrange",
        typlen: -1,
        typtype: 'b',
        typcategory: 'A',
        typarray: 0,
    },
    TypeFact {
        label: "TSRANGE_ARRAY",
        oid: oid::TSRANGE_ARRAY,
        typname: "_tsrange",
        typlen: -1,
        typtype: 'b',
        typcategory: 'A',
        typarray: 0,
    },
    TypeFact {
        label: "TSTZRANGE_ARRAY",
        oid: oid::TSTZRANGE_ARRAY,
        typname: "_tstzrange",
        typlen: -1,
        typtype: 'b',
        typcategory: 'A',
        typarray: 0,
    },
    TypeFact {
        label: "DATERANGE_ARRAY",
        oid: oid::DATERANGE_ARRAY,
        typname: "_daterange",
        typlen: -1,
        typtype: 'b',
        typcategory: 'A',
        typarray: 0,
    },
    TypeFact {
        label: "INT8RANGE_ARRAY",
        oid: oid::INT8RANGE_ARRAY,
        typname: "_int8range",
        typlen: -1,
        typtype: 'b',
        typcategory: 'A',
        typarray: 0,
    },
];

#[tokio::test]
async fn type_catalog_matches_live_postgres() {
    assert_eq!(TYPE_FACTS.len(), 67, "the oid.rs universe changed size");

    let Some(client) = connect().await else {
        print_skip_banner("TYPES", TYPE_FACTS.len(), "oids");
        return;
    };

    let oid_list = TYPE_FACTS
        .iter()
        .map(|f| f.oid.get().to_string())
        .collect::<Vec<_>>()
        .join(",");

    let rows = client
        .query(
            &format!(
                "SELECT oid, typname, typlen, typtype::text, typcategory::text, typarray \
                 FROM pg_type WHERE oid IN ({oid_list})"
            ),
            &[],
        )
        .await
        .expect("query pg_type for every oid.rs constant");

    struct Live {
        typname: String,
        typlen: i16,
        typtype: char,
        typcategory: char,
        typarray: u32,
    }
    let mut live: HashMap<u32, Live> = HashMap::new();
    for row in &rows {
        let oid_val: u32 = row.get(0);
        let typtype: String = row.get(3);
        let typcategory: String = row.get(4);
        live.insert(
            oid_val,
            Live {
                typname: row.get(1),
                typlen: row.get(2),
                typtype: typtype.chars().next().expect("typtype is one char"),
                typcategory: typcategory.chars().next().expect("typcategory is one char"),
                typarray: row.get(5),
            },
        );
    }

    let mut verified = 0usize;
    let mut array_pairs_checked = 0usize;
    let mut disagreements: Vec<String> = Vec::new();

    for fact in TYPE_FACTS {
        verified += 1;
        let Some(lv) = live.get(&fact.oid.get()) else {
            disagreements.push(format!(
                "{} (oid {}): no live pg_type row at all",
                fact.label, fact.oid
            ));
            continue;
        };
        if lv.typname != fact.typname {
            disagreements.push(format!(
                "{} (oid {}): typname Basin={:?} live={:?}",
                fact.label, fact.oid, fact.typname, lv.typname
            ));
        }
        if lv.typlen != fact.typlen {
            disagreements.push(format!(
                "{} (oid {}): typlen Basin={} live={}",
                fact.label, fact.oid, fact.typlen, lv.typlen
            ));
        }
        if lv.typtype != fact.typtype {
            disagreements.push(format!(
                "{} (oid {}): typtype Basin={:?} live={:?}",
                fact.label, fact.oid, fact.typtype, lv.typtype
            ));
        }
        if lv.typcategory != fact.typcategory {
            disagreements.push(format!(
                "{} (oid {}): typcategory Basin={:?} live={:?}",
                fact.label, fact.oid, fact.typcategory, lv.typcategory
            ));
        }
        if lv.typarray != fact.typarray {
            disagreements.push(format!(
                "{} (oid {}): typarray Basin={} live={}",
                fact.label, fact.oid, fact.typarray, lv.typarray
            ));
        }

        // The array pairing, cross-checked by OID IDENTITY (never by name —
        // `_int4` vs `INT4_ARRAY` would make a name comparison a
        // false-positive machine, per the module docs).
        if let Some(arr) = oid::array_of(fact.oid) {
            array_pairs_checked += 1;
            if arr.get() != lv.typarray {
                disagreements.push(format!(
                    "{} (oid {}): array_of() says {} but live typarray is {}",
                    fact.label, fact.oid, arr, lv.typarray
                ));
            }
            if oid::element_of(arr) != Some(fact.oid) {
                disagreements.push(format!(
                    "{} (oid {}): element_of(array_of(x)) = {:?}, does not round-trip",
                    fact.label,
                    fact.oid,
                    oid::element_of(arr)
                ));
            }
        }
    }

    eprintln!(
        "[type_matrix] TYPES — {verified} of {} oids verified (typname/typlen/typtype/typcategory/typarray) \
         against live pg_type · {array_pairs_checked} array pairings cross-checked by oid identity \
         · {} disagreements",
        TYPE_FACTS.len(),
        disagreements.len(),
    );
    for d in &disagreements {
        eprintln!("  {d}");
    }

    assert!(
        disagreements.is_empty(),
        "{} of {} type-catalog facts disagree with live pg_type:\n{}",
        disagreements.len(),
        TYPE_FACTS.len(),
        disagreements.join("\n")
    );
}
