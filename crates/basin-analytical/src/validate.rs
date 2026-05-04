//! SQL escape detection.
//!
//! DuckDB has built-in functions that take a filesystem path or URI directly:
//! `read_parquet`, `read_csv`, `read_json`, plus `ATTACH 'path'` and a handful
//! of extension functions. Allowing any of these from a tenant-submitted query
//! would let one tenant point DuckDB at another tenant's prefix (or, worse,
//! `/etc/passwd` on the analytical worker).
//!
//! v0.1 takes the crude-but-effective approach: substring-match the SQL,
//! case-insensitive, against a deny-list. Catalog-resolved table names go
//! through views, never as raw paths, so this filter has no impact on
//! legitimate analytical queries.
//!
//! v0.2 will replace this with a proper AST walk (DuckDB's `FROM read_parquet`
//! is parseable; we'd reject only the raw-function variant and let the rest
//! of DuckDB's surface through). For now, the deny-list is small enough that
//! one substring scan per query is free.

use basin_common::{BasinError, Result};

const FORBIDDEN: &[&str] = &[
    "read_parquet",
    "read_csv",
    "read_json",
    "read_ndjson",
    "parquet_scan",
    // ATTACH binds an external database file; not safe to expose.
    "attach",
    // COPY ... TO/FROM 'path' — same path-escape risk.
    "copy ",
];

/// Returns `Ok(())` if the SQL doesn't contain any string in the deny list.
/// Otherwise returns [`BasinError::InvalidIdent`] naming the offending token
/// so the caller can render a useful error message.
///
/// Matching is case-insensitive on ASCII. Whitespace and comments are not
/// stripped — a customer who hides `read_parquet` inside a `/* ... */` comment
/// still triggers the filter, which is the conservative behaviour for v0.1.
pub fn validate_sql(sql: &str) -> Result<()> {
    let lower = sql.to_ascii_lowercase();
    for needle in FORBIDDEN {
        if lower.contains(needle) {
            return Err(BasinError::InvalidIdent(format!(
                "analytical SQL may not contain {needle:?}"
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allows_normal_sql() {
        for sql in [
            "SELECT * FROM t",
            "SELECT count(*), avg(price) FROM orders WHERE id > 100",
            "SELECT a.x, b.y FROM a JOIN b ON a.id = b.id",
        ] {
            validate_sql(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
        }
    }

    #[test]
    fn rejects_forbidden() {
        for sql in [
            "SELECT * FROM read_parquet('/etc/passwd')",
            "select * from READ_PARQUET('x')",
            "SELECT * FROM read_csv('x')",
            "SELECT * FROM read_json('x')",
            "ATTACH 'x' AS y",
            "attach 'x' as y",
            "COPY t TO '/tmp/x.csv'",
        ] {
            let err = validate_sql(sql).unwrap_err();
            assert!(matches!(err, BasinError::InvalidIdent(_)), "{sql}: {err:?}");
        }
    }

    #[test]
    fn case_insensitive() {
        assert!(validate_sql("Read_Parquet('x')").is_err());
        assert!(validate_sql("READ_CSV('x')").is_err());
    }
}
